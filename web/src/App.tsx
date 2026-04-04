import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import CompaniesPage from './pages/Companies';
import CompanyDetailPage from './pages/CompanyDetail';
import ContactsPage from './pages/Contacts';
import ContactDetailPage from './pages/ContactDetail';
import DiscussionsPage from './pages/Discussions';
import DiscussionDetailPage from './pages/DiscussionDetail';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/companies" replace />} />
          <Route path="companies" element={<CompaniesPage />} />
          <Route path="companies/:id" element={<CompanyDetailPage />} />
          <Route path="contacts" element={<ContactsPage />} />
          <Route path="contacts/:email" element={<ContactDetailPage />} />
          <Route path="discussions" element={<DiscussionsPage />} />
          <Route path="discussions/:id" element={<DiscussionDetailPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
